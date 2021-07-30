import json
import pandas as pd
from pyvi import ViTokenizer

if __name__ == '__main__':
    fRead = open("D:\\ThucTap\\data\\ngoc_data_3.json", "r", encoding="utf8")
    fWrite = open("D:\\ThucTap\\data\\Result\\ngoc_data_3.json", "a", encoding="utf8")
    Lines = fRead.readlines()
    count = 0
    lamdaForReplaceCharacter = lambda x: x.replace("_", " ")
    lamdaForLowerCase = lambda x: x.lower()
    # Strips the newline character
    for line in Lines:
        count += 1
        if count % 2 != 0:
            fWrite.write(line)
        else:
            json_object = json.loads(line.strip())
            #print(json_object["title"])
            arrayPhrase = ViTokenizer.tokenize(json_object["title"]).split()
            arrayPhrase = filter(lambda x: x.find("_") != -1, arrayPhrase)
            arrayPhrase = list(map(lamdaForReplaceCharacter, arrayPhrase))
            arrayPhrase = list(map(lamdaForLowerCase, arrayPhrase))
            fWrite.write("{" + "\"suggest_title\": [\"" + '", "'.join(arrayPhrase) + "\"]}\n")
    fRead.close()
    fWrite.close()