#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Точечная порча индексной страницы для проверки полноты средства контроля.

Каждый вид порчи нарушает ровно один инвариант, чтобы можно было утверждать,
какая именно проверка сработала. Правки байтовые и делаются при остановленном
сервере.

Разметка (src/include/storage/bufpage.h, src/include/access/ginblock.h):
  смещение 12  pd_lower   uint16
  смещение 14  pd_upper   uint16
  смещение 16  pd_special uint16   — начало служебной области
  смещение 24  начало массива указателей на элементы, по 4 байта
  pd_special+0 rightlink  uint32
  pd_special+4 maxoff     uint16
  pd_special+6 flags      uint16

Контрольные суммы страниц должны быть отключены (initdb --no-data-checksums):
иначе порча будет поймана слоем контрольных сумм, а не проверкой инвариантов.
Это и есть верная постановка: дефект в самой СУБД оставил бы контрольную сумму
согласованной.
"""
import sys, struct

BLCKSZ = 8192
GIN_DATA, GIN_LEAF, GIN_DELETED, GIN_META = 1, 2, 4, 8

def rd(f, blk):
    f.seek(blk * BLCKSZ); return bytearray(f.read(BLCKSZ))

def wr(f, blk, page):
    f.seek(blk * BLCKSZ); f.write(bytes(page))

def opaque(page):
    sp = struct.unpack_from('<H', page, 16)[0]
    rl, maxoff, flags = struct.unpack_from('<IHH', page, sp)
    return sp, rl, maxoff, flags

def describe(page):
    sp, rl, maxoff, flags = opaque(page)
    lower = struct.unpack_from('<H', page, 12)[0]
    kinds = [n for b, n in ((GIN_DATA,'data'),(GIN_LEAF,'leaf'),
                            (GIN_DELETED,'deleted'),(GIN_META,'meta')) if flags & b]
    return f"flags={flags:#06x}({','.join(kinds) or 'internal-entry'}) rightlink={rl} maxoff={maxoff} items={(lower-24)//4}"

def find_page(f, nblocks, want_leaf, want_data):
    """Первая страница нужного вида, кроме метастраницы."""
    for blk in range(1, nblocks):
        page = rd(f, blk)
        if len(page) < BLCKSZ: break
        sp, rl, maxoff, flags = opaque(page)
        if flags & (GIN_DELETED | GIN_META): continue
        if bool(flags & GIN_LEAF) != want_leaf: continue
        if bool(flags & GIN_DATA) != want_data: continue
        lower = struct.unpack_from('<H', page, 12)[0]
        if not want_data and (lower - 24) // 4 < 3: continue   # нужно >=3 элемента
        return blk, page
    return None, None

def corrupt(path, kind):
    import os
    nblocks = os.path.getsize(path) // BLCKSZ
    with open(path, 'r+b') as f:
        if kind == 'order':
            # переставить два соседних указателя на элементы страницы словаря:
            # нарушается только упорядоченность элементов внутри страницы
            blk, page = find_page(f, nblocks, want_leaf=True, want_data=False)
            if blk is None: return None, 'подходящей страницы словаря не найдено'
            a = struct.unpack_from('<I', page, 24)[0]
            b = struct.unpack_from('<I', page, 28)[0]
            struct.pack_into('<I', page, 24, b); struct.pack_into('<I', page, 28, a)
            wr(f, blk, page); return blk, 'переставлены элементы 1 и 2'
        if kind == 'rightlink':
            # правая ссылка указывает в никуда: нарушается полнота цепочки
            blk, page = find_page(f, nblocks, want_leaf=True, want_data=False)
            if blk is None: return None, 'подходящей страницы не найдено'
            sp, rl, maxoff, flags = opaque(page)
            struct.pack_into('<I', page, sp, 0xFFFFFFF0)
            wr(f, blk, page); return blk, f'правая ссылка {rl} -> 4294967280'
        if kind == 'deleted':
            # страница помечена удалённой, но ссылка на неё остаётся:
            # нарушается недостижимость удалённых страниц
            blk, page = find_page(f, nblocks, want_leaf=True, want_data=True)
            if blk is None: return None, 'подходящей страницы дерева вхождений не найдено'
            sp, rl, maxoff, flags = opaque(page)
            struct.pack_into('<H', page, sp + 6, flags | GIN_DELETED)
            wr(f, blk, page); return blk, f'флаги {flags:#06x} -> {flags|GIN_DELETED:#06x}'
        if kind == 'parentkey':
            # изменить ключ во внутренней странице словаря: нарушается
            # согласованность ключа родителя — тот самый инвариант, вокруг
            # которого был дефект cdd1a431f21
            blk, page = find_page(f, nblocks, want_leaf=False, want_data=False)
            if blk is None: return None, 'внутренней страницы словаря не найдено'
            # берётся ПОСЛЕДНИЙ элемент страницы и ключ увеличивается: порядок
            # на странице при этом сохраняется, нарушается только покрытие
            # ключом родителя. Если брать первый элемент, ломается и порядок,
            # и тогда по срабатыванию нельзя сказать, какая проверка поймала.
            lower = struct.unpack_from('<H', page, 12)[0]
            nitems = (lower - 24) // 4
            if nitems < 2: return None, 'слишком мало элементов'
            itemid = struct.unpack_from('<I', page, 24 + 4 * (nitems - 1))[0]
            off = itemid & 0x7FFF          # lp_off — младшие 15 бит
            if off < 24 or off + 16 > BLCKSZ: return None, f'странное смещение {off}'
            old = struct.unpack_from('<i', page, off + 8)[0]
            struct.pack_into('<i', page, off + 8, old + 1000000)
            wr(f, blk, page)
            return blk, f'ключ последнего элемента {old} -> {old+1000000}'
        if kind == 'parentkey-down':
            # Ключ последнего элемента УМЕНЬШАЕТСЯ до значения чуть больше
            # предыдущего: порядок на странице сохраняется, но ключ родителя
            # перестаёт покрывать содержимое потомка. Увеличение ключа
            # ('parentkey') эту сторону не нарушает — родитель лишь объявляет
            # диапазон шире действительного, — поэтому оно проверкой и не
            # обязано ловиться.
            blk, page = find_page(f, nblocks, want_leaf=False, want_data=False)
            if blk is None: return None, 'внутренней страницы словаря не найдено'
            lower = struct.unpack_from('<H', page, 12)[0]
            nitems = (lower - 24) // 4
            if nitems < 3: return None, 'слишком мало элементов'
            off_last = struct.unpack_from('<I', page, 24 + 4 * (nitems - 1))[0] & 0x7FFF
            off_prev = struct.unpack_from('<I', page, 24 + 4 * (nitems - 2))[0] & 0x7FFF
            if min(off_last, off_prev) < 24: return None, 'странное смещение'
            prev = struct.unpack_from('<i', page, off_prev + 8)[0]
            old = struct.unpack_from('<i', page, off_last + 8)[0]
            if old <= prev + 1: return None, f'нет зазора между {prev} и {old}'
            struct.pack_into('<i', page, off_last + 8, prev + 1)
            wr(f, blk, page)
            return blk, f'ключ последнего элемента {old} -> {prev+1} (пред. {prev})'
        if kind == 'control-entry-leaf':
            # контроль посещения: заведомо ловимая порча на той же странице,
            # что и проверка правой ссылки
            blk, page = find_page(f, nblocks, want_leaf=True, want_data=False)
            if blk is None: return None, 'страницы не найдено'
            lower = struct.unpack_from('<H', page, 12)[0]
            struct.pack_into('<H', page, 12, lower + 4)
            wr(f, blk, page); return blk, f'pd_lower {lower} -> {lower+4}'
        if kind == 'control-data-leaf':
            # контроль посещения страницы дерева вхождений, на которой
            # ставится отметка удаления
            blk, page = find_page(f, nblocks, want_leaf=True, want_data=True)
            if blk is None: return None, 'страницы не найдено'
            lower = struct.unpack_from('<H', page, 12)[0]
            struct.pack_into('<H', page, 12, lower + 4)
            wr(f, blk, page); return blk, f'pd_lower {lower} -> {lower+4}'
        if kind == 'maxoff':
            # заниженное число элементов на странице дерева вхождений
            blk, page = find_page(f, nblocks, want_leaf=False, want_data=True)
            if blk is None: return None, 'внутренней страницы дерева вхождений не найдено'
            sp, rl, maxoff, flags = opaque(page)
            if maxoff < 2: return None, 'слишком мало элементов'
            struct.pack_into('<H', page, sp + 4, maxoff - 1)
            wr(f, blk, page); return blk, f'maxoff {maxoff} -> {maxoff-1}'
    return None, 'неизвестный вид порчи'

if __name__ == '__main__':
    if sys.argv[1] == 'describe':
        import os
        path = sys.argv[2]; n = os.path.getsize(path) // BLCKSZ
        with open(path, 'rb') as f:
            for blk in range(min(n, int(sys.argv[3]) if len(sys.argv) > 3 else 8)):
                print(blk, describe(rd(f, blk)))
    else:
        blk, msg = corrupt(sys.argv[2], sys.argv[1])
        print(f"{blk}|{msg}")
