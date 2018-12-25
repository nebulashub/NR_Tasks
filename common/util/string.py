class StringUtil(object):

    @staticmethod
    def is_empty(s):
        if s is None:
            return True
        if not isinstance(s, str):
            raise Exception("s is not a string")
        if len(s) == 0:
            return True
        return False
