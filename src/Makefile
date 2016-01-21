TAR=a.out
SRC=$(shell ls *.go)

$(TAR):$(SRC)
	go build -o $@ $(SRC)
clean:
	-rm $(TAR) log_hahaproxy.log
