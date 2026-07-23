def read_errors(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if "ERROR" in line.upper():
                yield line.strip()

# Проверяем
for error in read_errors("test.log"):
    print(error)