def two_sum(nums, target):
    seen = {} # здесь будет хеш-таблица(словарь)
    for i, num in enumerate(nums):
        complement = target - num
        # проверрка в словаре происходит Мгновенно (О(1)), без перебора!
        if complement in seen:
            return [seen[complement], i]
        seen[num] = i

# проверить здесь же:
print(two_sum([2, 7, 11, 15], 9)) # [0, 1]