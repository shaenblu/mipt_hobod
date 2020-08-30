import sys

def main(argv):
    current_key = None
    line = sys.stdin.readline()
    try:
        while line:
            key, key1, value = line.split("\t")
            print("%s\t%d" % (key, int(key1)))
            current_key = key
            line = sys.stdin.readline()
        if current_key:
            print("%s\t%d" % (current_key, int(key1)))
    except "end of line":
        return None


if __name__ == "__main__":
    main(sys.argv)