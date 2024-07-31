#include "duckdb/optimizer/timer_util.h"

/***************************************
 * Timer functions of the test framework
 ***************************************/
namespace duckdb {
typedef struct timespec timespec;
timespec diff(timespec start, timespec end)
{
	timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
}

timespec sum(timespec t1, timespec t2) {
	timespec temp;
	if (t1.tv_nsec + t2.tv_nsec >= 1000000000) {
		temp.tv_sec = t1.tv_sec + t2.tv_sec + 1;
		temp.tv_nsec = t1.tv_nsec + t2.tv_nsec - 1000000000;
	} else {
		temp.tv_sec = t1.tv_sec + t2.tv_sec;
		temp.tv_nsec = t1.tv_nsec + t2.tv_nsec;
	}
	return temp;
}

void printTimeSpec(timespec t, const char* prefix) {
	std::string str = prefix + std::to_string(t.tv_sec) + "." + std::to_string(t.tv_nsec) + " s";
	Printer::Print(str);
	//    printf("%s: %d.%09d\n", prefix, (int)t.tv_sec, (int)t.tv_nsec);
}

timespec tic( )
{
	timespec start_time;
	if (-1 == clock_gettime(CLOCK_REALTIME, &start_time)) {
		Printer::Print("Could not get clock time!");
		D_ASSERT(false);
	}
	return start_time;
}

void toc( timespec* start_time, const char* prefix )
{
	timespec current_time;
	if (-1 == clock_gettime(CLOCK_REALTIME, &current_time)) {
		Printer::Print("Could not get clock time!");
		D_ASSERT(false);
	}
	printTimeSpec( diff( *start_time, current_time ), prefix );
	*start_time = current_time;
}

std::chrono::high_resolution_clock::time_point chrono_tic() {
	return std::chrono::high_resolution_clock::now();
}

long chrono_toc(std::chrono::high_resolution_clock::time_point* start_time, const char* prefix, bool print) {
	auto current_time = std::chrono::high_resolution_clock::now();
	auto time_diff = duration_cast<std::chrono::microseconds>(current_time - *start_time).count();
	std::string str = prefix + std::to_string(time_diff) + " us";
	if (print)
		Printer::Print(str);
	*start_time = current_time;
	return time_diff;
}

void appendLineToFile(string filepath, string line) {
	std::ofstream file;
	//can't enable exception now because of gcc bug that raises ios_base::failure with useless message
	//file.exceptions(file.exceptions() | std::ios::failbit);
	file.open(filepath, std::ios::out | std::ios::app);
	if (file.fail())
		throw std::ios_base::failure(std::strerror(errno));

	//make sure write fails with exception if something is wrong
	file.exceptions(file.exceptions() | std::ios::failbit | std::ifstream::badbit);

	file << line << std::endl;
}
} // namespace duckdb