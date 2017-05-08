
class OAIPMHError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def set_message(self, message):
        self.message = message

    def as_xml_dict(self):
        return ({'error':{'@code':self.code, '#text':self.message}})

    def __repr__(self):
        return str(self)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return (u'OAIPMHError({0}): \'{1}\' '.format(self.code, self.message))
        
# badArgument (all verbs)
class BadArgumentError(OAIPMHError):
    def __init__(self, message=''):
        if not message:
            message = '''The request includes illegal arguments, is missing required 
                          arguments, includes a repeated argument, or values for arguments 
                          have an illegal syntax.'''
        OAIPMHError.__init__(self, 'badArgument', message)

# badVerb (N/A).
class BadVerbError(OAIPMHError):
    def __init__(self, message=''):
        if not message:
            message = '''Value of the verb argument is not a legal OAI-PMH verb, the 
                          verb argument is missing, or the verb argument is repeated.'''
        OAIPMHError.__init__(self, 'badVerb', message)
  
# badResumptionToken (ListIdentifiers, ListRecords, ListSets)
class BadResumptionTokenError(OAIPMHError):
        def __init__(self, message=''):
            if not message:
                message = '''The value of the resumptionToken argument is invalid or expired.'''
            OAIPMHError.__init__(self, 'badResumptionToken', message)

# cannotDisseminateFormat (GetRecord, ListIdentifiers, ListRecords)  
class CannotDisseminateFormatError(OAIPMHError):
        def __init__(self, message=''):
            if not message:
                message = '''The metadata format identified by the value given for the 
                              metadataPrefix argument is not supported by the item or by 
                              the repository.'''
            OAIPMHError.__init__(self, 'cannotDisseminateFormat', message)

# idDoesNotExist (GetRecordList, MetadataFormats)
class IdDoesNotExistError(OAIPMHError):
        def __init__(self, message=''):
            if not message:
                message = '''The value of the identifier argument is unknown or illegal 
                              in this repository.'''
            OAIPMHError.__init__(self, 'idDoesNotExist', message)

# noRecordsMatch (ListIdentifiers, ListRecords)
class NoRecordsMatchError(OAIPMHError):
        def __init__(self, message=''):
            if not message:
                message = '''The combination of the values of the from, until, set and 
                              metadataPrefix arguments results in an empty list.'''
            OAIPMHError.__init__(self, 'noRecordsMatch', message)

# noMetadataFormats (ListMetadataFormats)
class NoMetadataFormatsError(OAIPMHError):
        def __init__(self, message=''):
            if not message:
                message = '''There are no metadata formats available for the specified item.'''
            OAIPMHError.__init__(self, 'noMetadataFormats', message)

# noSetHierarchy (ListSets, ListIdentifiers, ListRecords)  
class NoSetHierarchyError(OAIPMHError):
        def __init__(self, message=''):
            if not message:
                message = '''The repository does not support sets.'''
            OAIPMHError.__init__(self, 'noSetHierarchy', message)

  