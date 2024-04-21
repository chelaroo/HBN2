bin:
	mkdir -p ../lib/
	g++ -O3 -c -fPIC patterndetect.cpp -o ../lib/patterndetect.o
	g++ -O3 -shared -Wl,-soname,libpatterndetect.so -o ../lib/libpatterndetect.so  ../lib/patterndetect.o
