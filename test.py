test = {'hi':123,'aaa':222}
file = open('test.txt', 'w')
file.write(str(test))
file.close()