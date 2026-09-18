import styled from 'styled-components';

export const Section = styled.section`
  margin-top: 16px;
  padding: 0 16px 16px;
`;

export const Panel = styled.div`
  display: grid;
  gap: 16px;
  padding: 16px;
  border: 1px solid ${({ theme }) => theme.table.td.borderTop};
  border-radius: 8px;
  color: ${({ theme }) => theme.default.color.normal};
  background: ${({ theme }) => theme.default.backgroundColor};
`;

export const Description = styled.p`
  margin: 0;
  color: ${({ theme }) => theme.table.td.color.normal};
`;

export const Controls = styled.div`
  display: flex;
  align-items: flex-end;
  gap: 16px;
  flex-wrap: wrap;
`;

export const ModeSelector = styled.fieldset`
  display: flex;
  align-items: center;
  gap: 16px;
  margin: 0;
  padding: 0;
  border: 0;

  legend {
    margin-bottom: 8px;
    font-size: 14px;
    font-weight: 500;
  }

  label {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
`;

export const Field = styled.div`
  display: grid;
  gap: 6px;
  min-width: 220px;
`;

export const Label = styled.label`
  font-size: 14px;
  font-weight: 500;
`;

export const NumberInput = styled.input`
  width: 140px;
  height: 32px;
  padding: 0 12px;
  border: 1px solid ${({ theme }) => theme.input.borderColor.normal};
  border-radius: 4px;
  color: ${({ theme }) => theme.input.color.normal};
  background: ${({ theme }) => theme.input.backgroundColor.normal};

  &:focus {
    outline: none;
    border-color: ${({ theme }) => theme.input.borderColor.focus};
  }
`;

export const ReplicaSelect = styled.select`
  width: min(360px, 100%);
  min-width: 280px;
  height: 32px;
  padding: 0 12px;
  border: 1px solid ${({ theme }) => theme.input.borderColor.normal};
  border-radius: 4px;
  color: ${({ theme }) => theme.input.color.normal};
  background: ${({ theme }) => theme.input.backgroundColor.normal};

  &:focus {
    outline: none;
    border-color: ${({ theme }) => theme.input.borderColor.focus};
  }
`;

export const ReplicaControls = styled.div`
  display: grid;
  gap: 8px;
  min-width: 360px;
`;

export const ReplicaRow = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
`;

export const InlineAction = styled.button`
  width: fit-content;
  padding: 4px 8px;
  border: 0;
  color: ${({ theme }) => theme.link.color};
  background: transparent;
  cursor: pointer;

  &:hover:not(:disabled) {
    text-decoration: underline;
  }

  &:focus-visible {
    outline: 2px solid ${({ theme }) => theme.input.borderColor.focus};
    outline-offset: 2px;
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

export const Hint = styled.span`
  color: ${({ theme }) => theme.table.td.color.normal};
  font-size: 12px;
`;

export const Message = styled.p`
  margin: 0;
  padding: 8px 12px;
  border-radius: 4px;
  width: fit-content;
`;

export const ErrorMessage = styled(Message)`
  color: ${({ theme }) => theme.input.error};
`;

export const SuccessMessage = styled(Message)`
  color: ${({ theme }) => theme.tag.color};
  background: ${({ theme }) => theme.tag.backgroundColor.green};
`;

export const Preview = styled.div`
  display: grid;
  gap: 16px;
  padding-top: 8px;
  border-top: 1px solid ${({ theme }) => theme.table.td.borderTop};
`;

export const ManualEditor = styled.div`
  display: grid;
  gap: 8px;
  overflow-x: auto;
`;

export const ConfirmationBody = styled.div`
  display: grid;
  gap: 16px;
`;

export const Warning = styled.p`
  margin: 0;
  padding: 12px;
  border-radius: 4px;
  color: ${({ theme }) => theme.default.color.normal};
  background: ${({ theme }) => theme.tag.backgroundColor.yellow};
`;

export const ConfirmationSummary = styled.dl`
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin: 0;

  div {
    display: grid;
    gap: 4px;
  }

  dt {
    color: ${({ theme }) => theme.table.td.color.normal};
    font-size: 12px;
  }

  dd {
    margin: 0;
    font-weight: 500;
  }
`;

export const ConfirmInput = styled.input`
  width: 100%;
  height: 32px;
  padding: 0 12px;
  border: 1px solid ${({ theme }) => theme.input.borderColor.normal};
  border-radius: 4px;
  color: ${({ theme }) => theme.input.color.normal};
  background: ${({ theme }) => theme.input.backgroundColor.normal};

  &:focus {
    outline: none;
    border-color: ${({ theme }) => theme.input.borderColor.focus};
  }
`;

export const ModalActions = styled.div`
  display: flex;
  gap: 8px;
`;

export const Summary = styled.div`
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
`;

export const SummaryItem = styled.div`
  display: grid;
  gap: 4px;

  strong {
    font-size: 18px;
  }
`;

export const Table = styled.table`
  width: 100%;
  border-collapse: collapse;

  th,
  td {
    padding: 10px 12px;
    text-align: left;
    border-bottom: 1px solid ${({ theme }) => theme.table.td.borderTop};
  }

  th {
    color: ${({ theme }) => theme.table.td.color.normal};
  }

  td {
    color: ${({ theme }) => theme.table.td.color.normal};
  }
`;

export const AdvancedPreview = styled.details`
  summary {
    cursor: pointer;
    width: fit-content;
  }

  pre {
    max-height: 280px;
    overflow: auto;
    padding: 12px;
    border-radius: 4px;
    background: ${({ theme }) => theme.code.backgroundColor};
  }
`;

export const Actions = styled.div`
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
`;
