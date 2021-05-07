import pandas as pd
import requests as req
from lxml import etree
from multiprocessing.pool import ThreadPool
from functools import partial


class S_W():
    def __init__(self, l, margin):
        self.list = l
        self.margin = margin
        self.df = pd.DataFrame(columns=[
                               "name", "date", "start", "h_price", "l_price", "f_price", "real_f_price", "volume", "margin"])

    def scrape(self, stock_id):
        # for stock_id in self.list:
        resp = req.get(
            f"https://hk.finance.yahoo.com/quote/{stock_id}.HK/history/")
        content = resp.content.decode()
        html = etree.HTML(content)
        num = len(html.xpath(
            '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/*/td[1]/span/text()'))
        if num != 0:
            # for i in range(1,num+1):
            # for i in range(2,4): ## ytd
            for i in range(1, 3):  # today
                to_append = []
                for j in range(9):
                    if j == 0:
                        to_append.append(stock_id)
                    elif j == 7:
                        try:
                            to_append.append(html.xpath(
                                f'/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{i}]/td[{j}]/span/text()')[0].replace(',', ''))
                        except:
                            to_append.append(0)
                    elif j == 8:
                        to_append.append('N')
                    else:
                        try:
                            to_append.append(html.xpath(
                                f'/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{i}]/td[{j}]/span/text()')[0].replace(',', ''))
                        except:
                            to_append.append(0)
                a_series = pd.Series(to_append, index=self.df.columns)
                self.df = self.df.append(a_series, ignore_index=True)
            # change type and check margin
            self.df[["start", "h_price", "l_price", "f_price", "real_f_price", "volume"]] = self.df[[
                "start", "h_price", "l_price", "f_price", "real_f_price", "volume"]].apply(pd.to_numeric)
            if (self.df[self.df['name'] == stock_id]['volume'].iloc[0] - self.df[self.df['name'] == stock_id]['volume'].iloc[1])/self.df[self.df['name'] == stock_id]['volume'].iloc[1] > self.margin and (self.df[self.df['name'] == stock_id]['volume'].iloc[0])*(self.df[self.df['name'] == stock_id]['real_f_price'].iloc[0]) > 100000000:
                self.df.loc[self.df['name'] == stock_id, "margin"] = 'Y'
    # return self.df

    def main(self):
        with ThreadPool(20) as pool:
            pool.map(self.scrape, self.list)
            pool.close()
            pool.join()
        return self.df


if __name__ == "__main__":
    #D[D["margin"] == 'Y']
    new = pd.DataFrame(columns=["name", "date", "start", "h_price",
                                "l_price", "f_price", "real_f_price", "volume", "margin"])
    for j in range(0, 10000, 500):
        temp_list = [str(i) for i in range(j, j+500)]
        Test = S_W(temp_list, 2)
        D = Test.main()
        new = new.append(D[D["margin"] == 'Y'], ignore_index=True)
    print(new)
