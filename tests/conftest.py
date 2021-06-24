# -*- coding: utf-8 -*-
"""conftest.py for mgenomicsremotemail."""
import sys
sys.path.append("/talizorah/mf/andrea_remote/mgenomicsremotemail/src")


class MockApp():

    def __init__(self, return_value=None):
        self.return_value = return_value

    def run(self):
        return self.return_value
