import re

while True:
    num = str(input("Number: "))
    num_l = re.findall('[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?',
                       num)
    for n in range(0, len(num_l)):
        num_l[n] = str(num_l[n])
        num_l[n] = num_l[n].replace(",", "")
    print(num_l)
