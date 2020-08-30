import sys

def main(argv):
    current_key = None
    line = sys.stdin.readline()
    sum = 0
    try:
        while line:
            key, value = line.split("\t", 1)
            if key != current_key:
                if current_key:
                    print("%s\t%d" % (current_key, sum))
                current_key = key
                sum = 0
            sum += int(value)
            line = sys.stdin.readline()
        if current_key:
            print("%s\t%d" % (current_key, sum))
    except "end of line":
        return None



if __name__ == "__main__":
    main(sys.argv)