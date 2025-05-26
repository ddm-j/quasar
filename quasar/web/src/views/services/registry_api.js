const BASE_URL = 'http://localhost:8080/' // API URL

export const updateAssetsForClass = async (classType, className) => {
    const response = await fetch(`${BASE_URL}internal/${encodeURIComponent(classType)}/${encodeURIComponent(className)}/update-assets`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();


    if (!response.ok) {
        // Use message from responseData if available, otherwise use a generic error
        const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}

export const updateAllAssets = async () => {
    const response = await fetch(`${BASE_URL}internal/update-all-assets`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();


    if (!response.ok) {
        // Use message from responseData if available, otherwise use a generic error
        const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}

export const getRegisteredClasses = async () => {
    const response = await fetch(`${BASE_URL}internal/classes/summary`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();

    if (!response.ok) {
        // Use message from responseData if available, otherwise use a generic error
        const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}
export const uploadCode = async (classType, file, secretsObject) => {
  const formData = new FormData();
  formData.append('file', file, file.name); // The third argument is the filename

  // Convert secrets array to a JSON string to send as a single field.
  const secretsString = JSON.stringify(secretsObject);
  formData.append('secrets', secretsString);

  const response = await fetch(`${BASE_URL}internal/${encodeURIComponent(classType)}/upload`, {
    method: 'POST',
    body: formData,
  });

  const responseData = await response.json();

  if (!response.ok) {
    // Use message from responseData if available, otherwise use a generic error
    const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return responseData;
};
export const deleteRegisteredClass = async (classType, className) => {
  const response = await fetch(`${BASE_URL}internal/delete/${encodeURIComponent(classType)}/${encodeURIComponent(className)}`, {
    method: 'DELETE',
    headers: {
      'Content-Type': 'application/json', // Optional for DELETE if no body, but good practice
    },
  });

  // DELETE requests might not always return a body, or might return an empty body on success (204 No Content)
  // Or they might return a JSON body with a success/error message.
  let responseData;
  const contentType = response.headers.get("content-type");
  if (contentType && contentType.indexOf("application/json") !== -1) {
    responseData = await response.json();
  } else {
    // If no JSON body, create a default object or use the status text
    responseData = { message: response.statusText, status: response.status };
  }

  if (!response.ok) {
    const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return responseData; // Contains { message: "...", class_name: "...", class_type: "..." } on success
                       // or { message: "...", ..., file_deletion_error: "..." } on partial success (207)
                       // or throws an error with { error: "..." }
};