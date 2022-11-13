class PreIntraPost:
    """Very simple wrapper for a triple of objects"""
    def __init__(self, pre, intra, post):
        self.pre = pre
        self.intra = intra
        self.post = post

    def apply(self, fnc, pre=True, intra=True, post=True):
        """
        Apply fnc to each self.pre, self.intra, self.post if pre, intra,
        post is True, respectively.
        """
        if pre and self.pre is not None:
            fnc(self.pre)
        if intra and self.intra is not None:
            fnc(self.intra)
        if post and self.post is not None:
            fnc(self.post)

    def __getitem__(self, k):
        if k == 'pre':
            return self.pre
        if k == 'intra':
            return self.intra
        if k == 'post':
            return self.post

