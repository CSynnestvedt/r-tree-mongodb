global_word_list = []

def main():
    with open("tmp.txt") as f:
        for line in f.readlines():
            newLine = "\"" + line.strip("./").strip("\n") + "\","
            global_word_list.append(newLine)


# open file in write mode
    with open(r'new_tmp.txt', 'w') as fp:
        for item in global_word_list:
            # write each item on a new line
            fp.write("%s\n" % item)



if __name__ == "__main__":
    main()