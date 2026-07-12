#Проверка на дубли и их удаление в coins
coins = ["btc", "eth", "btc", "sol", "eth", "btc"]

# Способо 1: через set() (одна строка)
unique_coins_1 = list(set(coins))
print("Способ 1 (set):", unique_coins_1)


# Способ 2: через цикл for
unique_coins_2 = []
for coin in coins:
	if coin not in unique_coins_2:
		unique_coins_2.append(coin)
print("Способ 2 (цикл):", unique_coins_2)
