num = "34325"
print(num)
if num.lstrip('-').isdigit():
    print("is Digit")
else:
    try:
        float(num)
        print("is float")
    except ValueError:
        print("is not number")
