# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from StringIO import StringIO


def show(fig, width=600):
    """ Renders a Matplotlib figure in Zeppelin.

    :param fig: a Matplotlib figure
    :param width: the width in pixel of the rendered figure, defaults to 600

    Usage example::

        import matplotlib.pyplot as plt
        from moztelemetry.zeppelin import show

        fig = plt.figure()
        plt.plot([1, 2, 3])
        show(fig)
    """
    img = StringIO()
    fig.savefig(img, format='svg')
    img.seek(0)
    print("%html <div style='width:{}px'>{}</div>".format(width, img.buf))
