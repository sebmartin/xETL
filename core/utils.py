def add_key(rows):
    return [
        [key] + row
        for key, row in enumerate(rows, 1)
    ]


