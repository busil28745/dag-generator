def test():
    try:
        print('test start')
        raise Exception
    except Exception as e:
        print('exception')
    return 'returned'

def go():
    a = test()
    if not a:
        print('test')
    # else:
    #     print('else')

go()