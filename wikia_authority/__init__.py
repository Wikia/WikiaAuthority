class MinMaxScaler:
    """
    Scales values from 0 to 1 by default
    """

    def __init__(self, vals, enforced_min=0, enforced_max=1):
        self.min = min(vals)
        self.max = max(vals)
        self.enforced_min = enforced_min
        self.enforced_max = enforced_max

    def scale(self, val):
        return (((self.enforced_max - self.enforced_min) * (val - self.min))
                / (self.max - self.min)) + self.enforced_min