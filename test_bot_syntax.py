import pathlib
import unittest


class BotSyntaxTests(unittest.TestCase):
    def test_bot_module_compiles(self):
        source = pathlib.Path(__file__).with_name("bot.py").read_text(encoding="utf-8")
        compile(source, "bot.py", "exec")


if __name__ == "__main__":
    unittest.main()
