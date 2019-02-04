def SumofIntegers(lb, ub):
    total = 0
    for i in range(lb,ub+1):
        total += i
        i += 1
    return total

print SumofIntegers(1,5)

l = [('A',100),('B',200),('C',300)]
x = map(lambda (x,y): (x , y * SumofIntegers(10,50)),l)

print x