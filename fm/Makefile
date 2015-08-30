CXX = g++-4.9.2
CXXFLAGS = -Wall -Wno-format -Wconversion -O3 -fPIC -std=c++0x -fopenmp
MAIN = fm
FILES = common.cpp timer.cpp
SRCS = $(FILES:%.cpp=src/%.cpp)
HEADERS = $(FILES:%.cpp=src/%.h)

all: $(MAIN)

fm: src/train.cpp $(SRCS) $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(SRCS)

clean:
	rm -f $(MAIN)
