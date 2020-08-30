import sys
import re

def getAnswerCount(words):
    for i, word in enumerate(words):
        if 'AnswerCount=\"' in word:
            return word[13:len(word)-1]



def getTags(words):
    result = []
    for i, word in enumerate(words):
        if 'Tags=\"' in word:
            str = re.sub("(\\&gt\\;|\\&lt\\;|\"){1,}", " ", word[6:])
            tags = str.split()
            for tag in tags:
                if len(tag) > 0:
                    result.append(tag)
    return result



def main(argv):
    line = sys.stdin.readline()
    try:
        while line:
            words = line.split()
            if '<row' in words[0]:
                for i, word in enumerate(words):
                    if 'PostTypeId' in word:
                        if '1' == word[12:13] and '0'== getAnswerCount(words):
                            tags = getTags(words)
                            for j, res in enumerate(tags):
                                print("%s\t%d" % (res, 1))
            line = sys.stdin.readline()
    except "end of line":
        return None



if __name__ == "__main__":
    main(sys.argv)