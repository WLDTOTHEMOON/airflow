import string 

def col_convert(col_num):
    alphabeta = string.ascii_uppercase[:26]
    alphabeta = [i for i in alphabeta] + [i + j for i in alphabeta for j in alphabeta]
    return alphabeta[col_num - 1]