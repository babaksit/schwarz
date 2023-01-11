class InvalidDateTimeFormatError(ValueError):
    """Used to indicate that the date time format is incorrect"""
    pass


class InvalidTemperatureRangeError(ValueError):
    """Used to indicate that the temperature is not in valid range"""
    pass


class InvalidCityNameError(ValueError):
    """Used to indicate that the city name is incorrect"""
    pass
