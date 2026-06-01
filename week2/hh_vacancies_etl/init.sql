-- распарсили JSON
CREATE TABLE IF NOT EXISTS vacancies (
    id SERIAL PRIMARY KEY,
    vacancy_id VARCHAR(50) UNIQUE NOT NULL,
    title VARCHAR(500),
    company VARCHAR(200),
    salary_from INT,
    salary_to INT,
    currency VARCHAR(3),
    city VARCHAR(100),
    skills TEXT,
    parsed_at TIMESTAMP DEFAULT NOW()
);

-- сырой JSON
CREATE TABLE IF NOT EXISTS raw_vacancies_json (
    id SERIAL PRIMARY KEY,
    vacancy_id VARCHAR(50) UNIQUE NOT NULL,
    raw_data JSONB,
    parsed_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_vacancies_jsonb ON raw_vacancies_json USING GIN (raw_data);

