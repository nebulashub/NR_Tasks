from threading import RLock


def autowired(type):
    """类实例管理工具，传入对象类型，返回对应的实例
    :param type: 实例类型. 必须要有@Service() 或者 @Component()装饰器
    :return: 返回实例
    """
    return Autowired.get(type)


def autowired_named(type, name):
    """类实例管理工具，按名称返回实例
    :param type: 实例类型. 必须要有@Service() 或者 @Component()装饰器
    :param name: 名称
    :return: 返回实例
    """
    return Autowired.get_named(type, name)


class Autowired(object):

    _generators = dict()
    _objects = dict()
    _named_objects = dict()
    _registered = False

    _lock = RLock()

    @classmethod
    def get(cls, clz):
        cls._reg()
        if clz not in cls._objects.keys():
            with cls._lock:
                if clz not in cls._objects.keys():
                    cls._objects[clz] = cls._generate(clz)
        return cls._objects[clz]

    @classmethod
    def get_named(cls, clz, name):
        cls._reg()
        k = str(clz) + "." + name
        if k not in cls._named_objects.keys():
            with cls._lock:
                if k not in cls._named_objects.keys():
                    cls._named_objects[k] = cls._generate(clz)
        return cls._named_objects[k]

    @classmethod
    def _generate(cls, clz):
        if clz not in cls._generators.keys():
            raise Exception(str(clz) + " not found")
        ps = list()
        with cls._lock:
            if cls._generators[clz] is not None:
                for c in cls._generators[clz]:
                    ps.append(Autowired.get(c))
        return clz(*ps)

    @classmethod
    def _reg_generator(cls, clz, params):
        with cls._lock:
            cls._generators[clz] = params

    @classmethod
    def _reg(cls):
        if cls._registered:
            return
        with cls._lock:
            if cls._registered:
                return
            cls._reg_generator(str, None)
            cls._reg_generator(str, None)
            cls._reg_generator(float, None)
            cls._reg_generator(int, None)
            cls._reg_generator(dict, None)
            cls._reg_generator(list, None)
            cls._reg_generator(tuple, None)
            cls._registered = True


class Component(object):

    def __init__(self, *args):
        if args is not None:
            self.params = list()
            for it in args:
                self.params.append(it)

    def __call__(self, cls, *args, **kwargs):
        Autowired._reg_generator(cls, self.params)
        return cls


class Service(object):

    def __init__(self, *args):
        if args is not None:
            self.params = list()
            for it in args:
                self.params.append(it)

    def __call__(self, cls, *args, **kwargs):
        Autowired._reg_generator(cls, self.params)
        return cls
