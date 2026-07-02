import asyncio
import re

from playwright_helper import AsyncBrowser, get_chrome_path, kill_browsers  # noqa

USERNAME = "13912345678"
PASSWORD = "123456"

captured = {"url": None, "cookie": None}


async def capture_cookies():
    kill_browsers("chrome.exe")
    # kill_browsers("msedge.exe")

    async with AsyncBrowser(endpoint="http://127.0.0.1:9222", executable_path=get_chrome_path(), devtools=False, user_data_dir="D:\\user_data") as browser:
        page = await browser.get_page()

        async def on_request(request):
            if "api/kernelspecs" in request.url:
                captured["url"] = request.url
                captured["cookie"] = await request.header_value("cookie")
                print(captured)

        page.on("request", on_request)

        await page.goto("https://www.joinquant.com/research")
        await page.wait_for_load_state("networkidle")

        if "login" in page.url:
            await page.wait_for_load_state("domcontentloaded")
            await page.get_by_role("textbox", name="手机号").fill(USERNAME)
            await page.get_by_placeholder("请输入密码").fill(PASSWORD)
            await page.get_by_label("阅读并接受聚宽用户协议及隐私政策").check()
            await page.get_by_role("button", name="登 录").click()
            await page.wait_for_load_state("networkidle")

        await page.wait_for_timeout(5000)  # 额外等待5秒让动态内容加载


async def jupyter():
    COOKIE = captured['cookie']

    file_path = "config.py"

    # 1. 读取文件内容
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 2. 用正则替换 COOKIE 的值（保留变量名和等号，只替换引号内的值）
    new_content = re.sub(
        r"(COOKIE\s*=\s*)'.*?'",
        rf"\1'{COOKIE}'",
        content,
        count=1  # 只替换第一个匹配项
    )

    # 3. 写回文件
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    print("已完成替换")


async def main():
    await capture_cookies()
    await jupyter()


if __name__ == '__main__':
    asyncio.run(main())
