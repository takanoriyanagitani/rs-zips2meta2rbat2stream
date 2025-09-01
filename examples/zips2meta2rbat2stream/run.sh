#!/bin/sh

zdir="./sample.d"

geninput(){
	echo generating input files...

	mkdir -p ./sample.d

	echo hw00 > ./sample.d/z00.txt
	echo hw01 > ./sample.d/z01.txt
	echo hw02 > ./sample.d/z02.txt

	echo hw10 > ./sample.d/z10.txt
	echo hw11 > ./sample.d/z11.txt
	echo hw12 > ./sample.d/z12.txt

	ls ./sample.d/z0?.txt | zip -@ -o ./sample.d/z0.zip
	ls ./sample.d/z1?.txt | zip -@ -o ./sample.d/z1.zip

}

test -f ./sample.d/z0.zip || geninput
test -f ./sample.d/z1.zip || geninput

export DIR_OF_ZIPS="${zdir}"
./zips2meta2rbat2stream
