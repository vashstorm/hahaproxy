TAR=haha
SRC=$(shell ls *.go)

$(TAR):$(SRC)
	go build -o $@ $(SRC)
	strip $(TAR)
	zip -r haha.zip haha
clean:
	-rm $(TAR)
