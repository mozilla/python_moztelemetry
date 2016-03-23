import unittest

from moztelemetry import standards

class TestStandards(unittest.TestCase):

    def test_in_sample(self):
        ping = {'clientId': ''}
        for divisor in (10, 100, 1000):
            self.failIf(standards.in_sample(ping, divisor))

        ping = {'clientId': '6fd3eb50-8bec-4b9c-8778-59406171312a'}
        self.assertRaises(ValueError, standards.in_sample, ping, 3)
        for divisor in (10, 100, 1000):
            self.failIf(standards.in_sample(ping, divisor))

        ping = {'clientId': '42fe5ab0-9b19-4d75-a9f1-0d57aada05d6'}
        for divisor in (10, 100, 1000):
            self.failUnless(standards.in_sample(ping, divisor))

if __name__ == '__main__':
    unittest.main()
