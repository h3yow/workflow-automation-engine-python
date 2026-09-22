"""冒烟测试：应用可导入、骨架可实例化。"""


def test_import_app():
    from main import app

    assert app is not None
