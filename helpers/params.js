/*
Unnesting parameters
*/

// unnest any parameter from the selected params array
const unnestParam = (keyName, paramsArray, dataType) => {
  if (typeof keyName !== 'string' || keyName.trim() === '') {
    throw new Error("unnestParam: 'keyName' is required and must be a non-empty string.");
  }
  if (typeof paramsArray !== 'string' || paramsArray.trim() === '') {
    throw new Error("unnestParam: 'paramsArray' is required and must be a non-empty string.");
  }

  if (dataType) {
    // return the value from the selected column
    if (dataType === 'string') {
      return `(select value.string_value from unnest(${paramsArray}) where key = '${keyName}')`;
    } else if (dataType === 'int' || dataType === 'int64') {
      return `(select value.int_value from unnest(${paramsArray}) where key = '${keyName}')`;
    } else if (dataType === 'double') {
      return `(select value.double_value from unnest(${paramsArray}) where key = '${keyName}')`;
    } else if (dataType === 'float' || dataType === 'float64') {
      return `(select value.float_value from unnest(${paramsArray}) where key = '${keyName}')`;
    }

    throw new Error(`unnestParam: Unsupported dataType '${dataType}'. Supported values are 'string', 'int', 'int64', 'double', 'float', and 'float64'.`);
  } else {
    // return the value from the column that has data, cast as string
    return `(select coalesce(value.string_value, cast(value.int_value as string), cast(value.double_value as string), cast(value.float_value as string)) from unnest(${paramsArray}) where key = '${keyName}')`;
  }
};

// event_params and session_params

// unnest a param from the event_params array
const unnestEventParam = (keyName, dataType) => {
  return unnestParam(keyName, 'event_params', dataType);
};

// unnest a param from the session_params array
const unnestSessionParam = (keyName, dataType) => {
  return unnestParam(keyName, 'session_params', dataType);
};

module.exports = {
  unnestEventParam,
  unnestSessionParam
};
