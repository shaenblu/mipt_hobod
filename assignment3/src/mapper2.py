import sys

def main(argv):
    line = sys.stdin.readline()
    try:
        while line:
            words = line.split()
            print("%s\t%d\t%d" % (words[0], int(words[1]))
            line = sys.stdin.readline()
    except "end of line":
        return None


if __name__ == "__main__":
    main(sys.argv)