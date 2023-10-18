class ConsoleChars:
    bd_ansi = "\033[1m"
    un_ansi = "\033[4m"
    bdun_ansi = f"{bd_ansi}{un_ansi}"
    no_ansi = "\033[0m"
    it_ansi = "\033[3m"
    bdunit_ansi = f"{bdun_ansi}{it_ansi}"

    @staticmethod
    def bd(input : str, wrap : bool = True) -> str :
        """Formats input string as Bold"""
        out = f"{ConsoleChars.bd_ansi}{input}"
        if wrap :
            out += ConsoleChars.no_ansi
        return out

    @staticmethod
    def it(input : str, wrap : bool = True) -> str :
        """Formats input string as italic"""
        out = f"{ConsoleChars.it_ansi}{input}"
        if wrap :
            out += ConsoleChars.no_ansi
        return out

    @staticmethod
    def un(input : str, wrap : bool = True) -> str :
        """Formats input string as underlined"""
        out = f"{ConsoleChars.un_ansi}{input}"
        if wrap :
            out += ConsoleChars.no_ansi
        return out


