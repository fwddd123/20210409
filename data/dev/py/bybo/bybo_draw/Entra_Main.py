#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback



'''
//                              _ooOoo_
//                             o8888888o
//                             88" . "88
//                             (| -_- |)
//                              O\ = /O
//                           ____/`---'\____
//                        .   ' \\| |// `.
//                         / \\||| : |||// \
//                        / _||||| -:- |||||- \
//                         | | \\\ - /// | |
//                       | \_| ''\---/'' | |
//                        \ .-\__ `-` ___/-. /
//                    ___`. .' /--.--\ `. . __
//                  ."" '< `.___\_<|>_/___.' >'"".
//                 | | : `- \`.;`\ _ /`;.`/ - ` : | |
//                    \ \ `-. \_ __\ /__ _/ .-` / /
//           ======`-.____`-.___\_____/___.-`____.-'======
//                              `=---='
//
//           .............................................
//                     佛祖保佑             永无BUG
项目名称: 异步调取py测试
文件名称: Entra_Main.py
描述: 
创建时间: 
@author 
@version 
@update 
'''
import asyncio

# async def count():
#     print("One")
#     await asyncio.sleep(1)
#     print("Two")
#
# async def main():
#     await asyncio.gather(count(), count(), count())
#
# if __name__ == "__main__":
#     import time
#     s = time.perf_counter()
#     asyncio.run(main())
#     elapsed = time.perf_counter() - s
#     print(f"{__file__} executed in {elapsed:0.2f} seconds.")


@asyncio.coroutine
def hello():
    print("Hello world!")
    # 异步调用asyncio.sleep(1):
    r = yield from asyncio.sleep(1)
    print("Hello again!")
    k = yield from asyncio.sleep(1)
    print("Hello again!!")



# 获取EventLoop:
loop = asyncio.get_event_loop()
# 执行coroutine
loop.run_until_complete(hello())
loop.close()