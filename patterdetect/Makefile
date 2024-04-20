bin:
	mkdir -p ../lib/
	g++ -c -fPIC patterndetect.cpp -o ../lib/patterndetect.o
	g++ -shared -Wl,-soname,libpatterndetect.so -o ../lib/libpatterndetect.so  ../lib/patterndetect.o
