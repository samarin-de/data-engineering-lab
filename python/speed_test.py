import time
import random

# 1. Создаем список из 1 миллиона случайных чисел
big_list = list(range(1_000_000))
# Перемешиваем, чтобы искомое было где-то в середине
random.shuffle(big_list)

# 2. Ищем число 500000

# Способ 1: Поиск в списке (медленно, O(n))
start = time.time()
found = 500000 in big_list # Python вынужден перебрать всё!
end = time.time()
print(f"Поиск в СПИСКЕ (миллион элементов): {end - start:.6f} секунд")

# 3. Превращаем список во множество (хещ-таблица)
big_list = set(big_list)

# Способ 2: Поиск во множестве (мгновенно, O(1))
start = time.time()
found = 500000 in big_list # Python вычисляет хеш и идет по адресу!
end = time.time()
print(f"Поиск во МНОЖЕСТВЕ (хеш): {end - start:.6f} секунд")