# 项目根目录 / main.py
from ui.main import ChipInSightApp

# 必须用这个格式，NiceGUI 才能正常启动
if __name__ in {"__main__", "__mp_main__"}:
    app = ChipInSightApp()
    app.run()