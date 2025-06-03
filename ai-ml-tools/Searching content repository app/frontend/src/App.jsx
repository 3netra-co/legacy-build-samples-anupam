import React, { useState } from 'react';

function App() {
  const [question, setQuestion] = useState('');
  const [answer, setAnswer] = useState('');

  const askQuestion = async (e) => {
    e.preventDefault();
    const res = await fetch('http://127.0.0.1:5000/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question }),
    });
    const data = await res.json();
    setAnswer(data.answer);
  };

  return (
    <div style={{ padding: 40 }}>
      <h2>Ask a Question</h2>
      <form onSubmit={askQuestion}>
        <textarea value={question} onChange={e => setQuestion(e.target.value)} rows={4} cols={60} />
        <br /><br />
        <button type="submit">Submit</button>
      </form>
      <hr />
      <h3>Answer:</h3>
      <p>{answer}</p>
    </div>
  );
}

export default App;