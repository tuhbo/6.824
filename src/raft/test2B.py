import os

ntests = 50
nfails = 0
noks = 0

if __name__ == "__main__":
    for i in range(ntests):
        print("*************ROUND " + str(i+1) + "/" + str(ntests) + "*************")

        filename = "out" + str(i+1)
        os.system("go test -run 2B | tee " + filename)
        with open(filename) as f:
            if 'FAIL' in f.read():
                nfails += 1
                print("✖️fails, " + str(nfails) + "/" + str(ntests))
                continue
            else:
                noks += 1
                print("✔️ok, " + str(noks) + "/" + str(ntests))
                os.system("rm " + filename)