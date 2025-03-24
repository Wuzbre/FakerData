path = ["/a/b/c/../","/home/./user/../folder","/./x/y/../../z"]
def getResult(arr):
    result = []
    for i in arr:
        stack = list()
        strs = i.split("/")
        for i in strs:
            if i == '.':
                continue
            if i == '..':
                stack.pop()
            else:
                stack.append(i)
        result.append('/'.join(stack).removesuffix("/"))
    return result

print(getResult(path))