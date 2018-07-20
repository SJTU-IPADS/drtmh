#include "gtest/gtest.h"

#include "tx_operator.hpp"


using namespace nocc::rtx;


TEST(tx_test,get_op_test) {

  TXOpBase op_set;
  char buf[1024];

  struct TestStruct {
    int x;
    int y;
    inline TestStruct(int x,int y): x(x),y(y) {}
  };

  op_set.remote_rpc_op<TestStruct>(0,buf,NULL,12,14);
  TestStruct *s = (TestStruct *)buf;
  ASSERT_EQ(s->x,12);
  ASSERT_EQ(s->y,14);

  op_set.remote_rpc_op<TestStruct>(0,buf + sizeof(TestStruct),NULL,50,64);
  ASSERT_EQ(s[1].x,50);
  ASSERT_EQ(s[1].y,64);
}
