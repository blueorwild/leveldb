#include <fstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

void test(int **p){
  *p = new int(3);
}

using namespace std;
int main() {
  vector<int*> hehe(5);
  test(&hehe[3]);
  cout << *(hehe[3]) << endl;
  
  fstream file_in("./file.txt",ios::in);
  int fd1 = ::open("./file.txt", O_WRONLY | O_CREAT, 0644);
  if (fd1 < 0) {
    cout << "打开文件 写 失败" << endl;
  } else {
    ::lseek(fd1, 6, SEEK_SET);
    ::write(fd1, "mouzp", 5);
  }
  int fd2 = ::open("./file.txt", O_RDONLY);
  char buf[100] = {'\0'};
  if (fd2 < 0) {
    cout << "打开文件 读 失败" << endl;
  } else {
    ::pread(fd2, buf, 20, 0);
    cout << buf << endl;
  }

  ::close(fd1);
  ::close(fd2);
  return 0;
}