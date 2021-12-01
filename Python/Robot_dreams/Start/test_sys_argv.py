import sys  # first import built-in sys module.

argv_len = len(sys.argv)  # get command line argument length.
print('there are ' + str(argv_len) + ' arguments.')

for i in range(argv_len):  # loop in all arguments.
    tmp_argv = sys.argv[i]

    print(str(i))  # print out argument index and value.
    print(tmp_argv)
    print('argv ' + str(i) + ' = ' + tmp_argv)
