SELECT distinct 
    name,
    url
from {{ref('stg_gpu')}}
WHERE (directx NOT LIKE '12%') OR (opengl < 4.6)