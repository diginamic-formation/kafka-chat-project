import re


name_to_test = "#test"
name_sans_diese = name_to_test[1:]
print(name_sans_diese)
regex = "^[a-zA-Z0-9-]+$"
print(re.match(regex, name_to_test[1:]))

