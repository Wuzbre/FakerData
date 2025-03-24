arr = [2, 3, 1, 2, 4, 3]
target = 7


def getresult(arr, target):
    x = len(arr)
    tmp = float('inf')
    l = 0
    s = 0
    for i in range(x):
        s += arr[i]
        while s >= target:
            tmp = min(tmp, i - l + 1)
            s -= arr[l]
            l += 1
    return tmp if tmp <= x else 0
"""
s   tmp l
2   4   0
3   4   1
1   4   2
2

4

3
"""


print(getresult(arr, target))
