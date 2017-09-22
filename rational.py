from collections import namedtuple

from util import gcd


class AdditiveMixin:
    def __add__(self, other):
        if isinstance(other, int):
            other = self.from_int(other)

        if isinstance(other, Rational):
            new_num = self.num * other.denom + other.num * self.denom
            new_denom = self.denom * other.denom
            return Rational(new_num, new_denom)

        return NotImplemented

    def __neg__(self):
        return Rational(-self.num, self.denom)


class Rational(namedtuple('Rational', ['num', 'denom']), AdditiveMixin):
    def __new__(cls, num, denom):
        if denom == 0:
            raise ValueError('Denominator cannot be null')
        factor = gcd(num, denom)
        if denom < 0:
            num, denom = -num, -denom
        return super().__new__(cls, num // factor, denom // factor)

    def __str__(self):
        return '{}/{}'.format(self.num, self.denom)

    @staticmethod
    def from_int(number):
        number = Rational(number, 1)
        return number

    def __radd__(self, other):
        return self + other

    def __sub__(self, other):
        return self + (-other)

    def __rsub__(self, other):
        return -self + other