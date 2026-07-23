def is_anagram(s, t):
    if len(s) != len(t):
        return False
    count = {}
    
    # Подсчет букв в s
    for ch in s:
        if ch in count:
            count[ch] += 1
        else:
            count[ch] = 1
    
    # Проверка и уменьшение по t
    for ch in t:
        if ch not in count:
            return False
        count[ch] -= 1
        if count[ch] < 0:
            return False

    return True

# Проверка 
print(is_anagram("anagram", "nagaram")) # True
print(is_anagram("rat", "car"))         # False
print(is_anagram("a", "ab"))            # False (разная длина)