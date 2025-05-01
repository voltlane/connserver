
all: bin/test-client-c

.PHONY: all clean

bin/test-client-c: main.c bin/libclientcom_c.a
	${CC} -o $@ $< -Lbin -lclientcom_c -I../../clientcom-c/include -lm -g -O3 -flto

bin/libclientcom_c.a: ../../target/release/libclientcom_c.a
	mkdir -p bin
	cp $< $@

bin:
	mkdir -p bin

clean:
	rm -rf bin
