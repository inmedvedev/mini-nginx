class Timeouts:
    def __init__(self, cfg):
        self.connect = cfg.connect_ms / 1000
        self.read = cfg.read_ms / 1000
        self.write = cfg.write_ms / 1000
        self.total = cfg.total_ms / 1000