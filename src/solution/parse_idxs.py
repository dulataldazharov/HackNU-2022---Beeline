def find_identifiers(s, first_only=False):
    idxs = set()

    def is_left_allowed(c):
        if c.isalpha():
            return True
        
        if c.isdigit():
            return True
        
        if c in ["_"]:
            return True
        
        return False


    def is_right_allowed(c):
        if c in ["L"]:
            return True
        
        if c.isdigit():
            return True

        return False

    for i in range(len(s)):
        if s[i] != '#':
            continue

        l = i - 1
        r = i + 1

        while l >= 0 and is_left_allowed(s[l]):
            l = l - 1

        while r < len(s) and is_right_allowed(s[r]):
            r = r + 1
        
        if l != i - 1 and r != i + 1:
            if first_only:
                return s[l+1:r]
            idxs.add(s[l+1:r])

    if first_only:
        return None

    return list(idxs)


# with open("input.txt", "r") as f:
#     result = get_indicies(f.read())

#     for id in result:
#         print(id)