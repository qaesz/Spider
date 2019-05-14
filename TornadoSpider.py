from pyquery import PyQuery as pq
from tornado import ioloop, gen, httpclient, queues
from urllib.parse import urljoin
import time

class TornadoScrapy():
    def __init__(self,url):
        self.base_url = url
        self.concurrency = 8


    async def get_url_links(self,url): # 页面数据 和 next_page_url
        response = await httpclient.AsyncHTTPClient().fetch(url)
        html = response.body.decode("utf-8")
        p = pq(html)(".list-content") # 初始url截取范围
        # print(p(".qy-mod-ul"))
        links = [] # 爬取链接
        for i in range(10000): # 循环链接
            if str(p("a").eq(i)).strip(): # strip() 方法用于移除字符串头尾指定的字符（默认为空格或换行符）或字符序列。
                # 该方法只能删除开头或是结尾的字符，不能删除中间部分的字符。

                # links.append(urljoin(base_url, p("a").eq(i).attr("href"))) # 页面所有链接

                if str(p("a").eq(i).attr("href")).find("v_")!=-1: # 过滤链接
                    # urljoin()，第一个参数是基础母站的url，第二个是需要拼接成绝对路径的url
                    links.append(urljoin(self.base_url, p("a").eq(i).attr("href")))

                continue
            break

        return links


    async def main(self):
        seen_set = set()
        q = queues.Queue()

        async def fetch_url(current_url):
            if current_url in seen_set:
                return

            if self.base_url!=current_url:
                print(f"获取：{current_url}")
            # print(f"入口：{self.base_url}")
            seen_set.add(current_url)

            next_urls = await self.get_url_links(current_url)
            for next_url in next_urls:
                # if next_url.startswith(base_url):
                await q.put(next_url)

        async def worker():
            async for url in q:
                if url is None:
                    return
                try:
                    await fetch_url(url)
                except Exception as e:
                    print(f"exception:{e}")
                finally:
                    # 计数器，每进入一个就加1，所以我们调用完了之后，要减去1
                    q.task_done()

        # 放入初始url到队列
        await q.put(self.base_url)

        # 启动协程，同时开启三个消费者
        workers = gen.multi([worker() for _ in range(3)])

        # 会阻塞，直到队列里面没有数据为止
        await q.join()

        for _ in range(self.concurrency):
            await q.put(None)

        # 等待所有协程执行完毕
        await workers

    def run(self):
        ioloop.IOLoop.current().run_sync(self.main)



if __name__ == '__main__':    
    start = time.time()
    for i in range(1,20):
        url = 'https://list.iqiyi.com/www/1/-------------11-'+str(i)+'-1-iqiyi--.html'
        run = TornadoScrapy(url)
        run.run()
    print(time.time() - start)


