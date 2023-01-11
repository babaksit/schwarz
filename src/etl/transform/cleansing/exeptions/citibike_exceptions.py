class InvalidRideIdLengthError(ValueError):
    """Used to indicate that the ride id length is incorrect"""
    pass


class InvalidRideIdDataError(ValueError):
    """Used to indicate that the ride id data is incorrect"""
    pass


class InvalidBikeTypeError(TypeError):
    """Used to indicate that the ride id type is incorrect"""
    pass


class InvalidDateTimeFormatError(ValueError):
    """Used to indicate that the date time format is incorrect"""
    pass


class InvalidStationNameError(ValueError):
    """Used to indicate that the date time format is incorrect"""
    pass


class InvalidStationIdError(ValueError):
    """Used to indicate that the station id is incorrect"""
    pass


class StartTimeIsGEEndtimeError(ValueError):
    """Used to indicate that the start time is greater than the end time"""
    pass


class InvalidLatRangeError(ValueError):
    """Used to indicate that the Latitude range is invalid"""
    pass


class InvalidLonRangeError(ValueError):
    """Used to indicate that the Longitude range is invalid"""
    pass


class InvalidUserTypeError(ValueError):
    """Used to indicate that the user type is invalid"""
    pass
